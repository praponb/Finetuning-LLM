from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("revenue_per_unit", "id")
w_running = Window.partitionBy(F.date_trunc("month", "sale_date"), "city").orderBy(F.col("revenue_per_unit").desc_nulls_first())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-07-29")))
    .select(
        "*",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "inner")
    .select("src.*", F.col("s.city"))
)

base = (
    with_dims
    .filter(F.col("city").isNotNull())
    .withColumn("revenue_per_unit_times_units", (F.col("revenue_per_unit") * F.col("units")))
)

scored = (
    base
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("revenue_per_unit_percent_rank", F.percent_rank().over(w_running))
)

joined = (
    scored
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.launch_date"), F.col("p.stock_qty"), F.col("p.category"))
)

result = (
    joined
    .select(
        "id",
        "store_id",
        "revenue_per_unit_percent_rank",
        "revenue",
        F.col("revenue_per_unit_times_units").alias("REVENUE_PER_UNIT_TIMES_UNITS_VAL"),
        "revenue_per_unit",
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["revenue_per_unit_percent_rank", "revenue", "REVENUE_PER_UNIT_TIMES_UNITS_VAL", "revenue_per_unit"],
        variableColumnName="metric_name",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
