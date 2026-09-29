from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_start_date_quarter_to_date_h = Window.partitionBy(F.to_date("start_date_quarter"), "holder_id")
w_start_date_month_holder_id_b = Window.partitionBy(F.date_trunc("month", "start_date"), "holder_id").orderBy("premium", "id")

recent_policys = (
    df_policies
    .filter(F.col("product").isin("HEALTH", "HOME", "AUTO"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        F.abs(F.col("premium") - 2319.8).alias("premium_distance"),
        F.coalesce(F.col("premium"), F.lit(0.0)).alias("premium_filled"),
        F.date_trunc("quarter", F.col("start_date")).alias("start_date_quarter"),
    )
)

windowed = (
    recent_policys
    .withColumn("seq_no", F.row_number().over(w_start_date_month_holder_id_b))
    .withColumn("rows_in_start_date_quarter_to_date_holder_id", F.count(F.lit(1)).over(w_start_date_quarter_to_date_h))
)

result = (
    windowed
    .select(
        "id",
        "premium_distance",
        "premium_filled",
        "premium",
    )
    .unpivot(
        ids=["id"],
        values=["premium_distance", "premium_filled", "premium"],
        variableColumnName="field",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
    .orderBy("id", "field")
)
