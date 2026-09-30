from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("week", "sale_date_period")).orderBy(F.col("sale_date_period").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w2 = Window.orderBy(F.col("revenue_per_unit").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy("sale_date_period", "id")
w4 = Window.partitionBy("product_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy(F.date_trunc("week", "opened_date"), "product_id").orderBy(F.col("session_no").desc(), "id")
units_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("units").alias("avg_store_sale_units"))

base = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-14").cast("date"))
    .select("*", F.ceil("revenue").alias("revenue_whole"))
)

cleaned = (
    base
    .filter(F.year("sale_date") >= 2024)
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "revenue_whole",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        F.date_trunc("year", "sale_date").alias("sale_date_period"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        "revenue_whole",
        "revenue_per_unit",
        "sale_date_period",
        F.avg("revenue_per_unit").over(w).alias("revenue_per_unit_avg_7d"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_dims = (
    metrics
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.opened_date"), F.col("s.store_name"))
)

valid = (
    with_dims
    .filter(F.col("revenue_per_unit_avg_7d") < 23)
    .withColumn("store_name_upper", F.upper("store_name"))
)

flagged = (
    valid
    .withColumn("new_group_flag", F.when(F.col("sale_date_period") - F.lag("sale_date_period").over(w3) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = flagged.withColumn("session_no", F.sum("new_group_flag").over(w4))

metrics_store_sales = (
    islands
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "revenue_whole",
        "sale_date_period",
        "revenue_per_unit_avg_7d",
        "opened_date",
        "new_group_flag",
        F.min("units").over(w5).alias("running_min_units"),
        F.avg("units").over(w5).alias("running_avg_units"),
        F.sum("units").over(w5).alias("running_sum_units"),
        F.row_number().over(w6).alias("row_seq_2"),
    )
)

result = (
    metrics_store_sales
    .join(units_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "revenue",
        "revenue_whole",
        "revenue_per_unit_avg_7d",
        "opened_date",
        "new_group_flag",
        "running_min_units",
        "running_avg_units",
        F.col("avg_store_sale_units"),
    )
    .orderBy(F.col("id").desc_nulls_last(), F.col("avg_store_sale_units").desc())
)
