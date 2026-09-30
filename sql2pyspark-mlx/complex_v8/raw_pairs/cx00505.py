from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_month = Window.partitionBy("opened_on_month")
w_opened_on_month_by_opened_on = Window.partitionBy("opened_on_month").orderBy("opened_on_month", "id").rowsBetween(-3, -1)

prepared_warehouses = (
    df_warehouses
    .filter(F.col("capacity") <= 11768)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.date_trunc("year", F.col("opened_on")).alias("opened_on_year"),
        F.month(F.col("opened_on")).alias("opened_on_month"),
        F.abs(F.col("capacity") - 4912).alias("capacity_distance"),
    )
)

labeled = (
    prepared_warehouses
    .filter(F.col("capacity_distance") <= 16000)
    .withColumn("opened_on_year_label", F.date_format(F.col("opened_on_year"), "yyyy-MM"))
)

filtered = (
    labeled
    .crossJoin(labeled.agg(F.avg("capacity_distance").alias("overall_avg_capacity_distance")))
    .filter(F.col("capacity_distance") > F.col("overall_avg_capacity_distance"))
    .drop("overall_avg_capacity_distance")
)

with_trends = (
    filtered
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on_month",
        "capacity_distance",
        F.count(F.lit(1)).over(w_opened_on_month).alias("rows_in_opened_on_month"),
        F.round(F.avg("opened_on_month").over(w_opened_on_month_by_opened_on), 2).alias("opened_on_month_avg_3_prior"),
    )
)

result = (
    with_trends
    .select(
        "id",
        F.col("capacity_distance").alias("capacity_distance_v"),
        F.col("capacity").alias("m_capacity"),
        F.col("opened_on_month").alias("m_opened_on_month"),
        F.col("rows_in_opened_on_month").alias("rows_in_opened_on_month_v"),
    )
    .unpivot(
        ids=["id"],
        values=["capacity_distance_v", "m_capacity", "m_opened_on_month", "rows_in_opened_on_month_v"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
