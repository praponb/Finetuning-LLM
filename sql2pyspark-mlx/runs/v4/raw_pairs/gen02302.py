from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("units").asc_nulls_first())
w3 = Window.partitionBy("store_id", "product_id").orderBy("revenue_per_unit", "id").rowsBetween(-2, -1)
w_grp = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_per_unit").desc())
w5 = Window.partitionBy("store_id").orderBy("sale_date_period", "id")
w6 = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("prev_sale_date_period", "id")
w8 = Window.partitionBy("product_id").orderBy("prev_sale_date_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter((F.col("sale_date") >= F.to_date(F.lit("2024-03-01"))) & (F.col("store_id") == 2))
    .select("*", (F.col("units") - F.col("revenue")).alias("revenue_per_unit"))
)

metrics = (
    base
    .select(
        "*",
        F.sum("units").over(w).alias("cumulative_units"),
        F.dense_rank().over(w2).alias("units_dense_rank"),
        F.avg("revenue").over(w3).alias("revenue_avg_2_prior"),
        F.percent_rank().over(w_grp).alias("revenue_per_unit_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("revenue_per_unit") < 33)
    .withColumn("revenue_avg_2_prior_level", F.when(F.col("revenue_avg_2_prior") >= 3749, "High").when(F.col("revenue_avg_2_prior") >= 1544, "Medium").otherwise("Low"))
    .withColumn("sale_date_period", F.date_trunc("year", "sale_date"))
)

with_dims = (
    valid
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.stock_qty"))
)

with_prev = with_dims.withColumn("prev_sale_date_period", F.lag("sale_date_period").over(w5))

with_gaps = (
    with_prev
    .withColumn("session_start_flag", F.when(F.col("prev_sale_date_period").isNull() | (F.col("sale_date_period") > F.col("prev_sale_date_period") + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = with_gaps.withColumn("burst_id", F.sum("session_start_flag").over(w6))

flagged = (
    sessionized
    .withColumn("is_new_burst", F.when(F.lag("prev_sale_date_period").over(w7).isNull() | (F.col("prev_sale_date_period") > F.lag("prev_sale_date_period").over(w7) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized_store_sales = flagged.withColumn("session_id", F.sum("is_new_burst").over(w8))

result = (
    sessionized_store_sales
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "revenue_per_unit_percent_rank",
        "revenue_avg_2_prior_level",
        "stock_qty",
        "prev_sale_date_period",
        "session_start_flag",
    )
    .orderBy(F.col("stock_qty").desc())
)
