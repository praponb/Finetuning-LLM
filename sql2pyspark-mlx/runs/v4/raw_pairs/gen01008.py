from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("revenue", "id").rowsBetween(-6, -1)
w_hist = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("store_id").orderBy("revenue_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy("revenue_filled", "id")
w6 = Window.partitionBy("region").orderBy("running_units", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("product_id").orderBy("sqft", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy("prev_revenue_whole", "id").rowsBetween(-2, -1)

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2024-04-10").cast("date")) & (F.col("sale_date") > "2024-12-01"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

base = (
    valid
    .filter(F.col("revenue_filled") < 74)
    .withColumn("revenue_whole", F.floor("revenue"))
)

joined = (
    base
    .alias("s")
    .join(df_stores.alias("p"), F.col("s.store_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.sqft"), F.col("p.region"), F.col("p.city"))
)

enriched = (
    joined
    .withColumn("running_units", F.sum("units").over(w))
    .withColumn("revenue_filled_avg_6_prior", F.avg("revenue_filled").over(w2))
    .withColumn("last_known_revenue_whole", F.last("revenue_whole", ignorenulls=True).over(w_hist))
    .withColumn("running_units_2", F.sum("units").over(w_recent))
    .withColumn("prev_revenue_whole", F.lag("revenue_whole", 1).over(w5))
)

enriched_store_sales = (
    enriched
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w6))
    .withColumn("revenue_filled_avg_6_prior_avg_3", F.avg("revenue_filled_avg_6_prior").over(w7))
    .withColumn("revenue_filled_avg_6_prior_avg_2_prior", F.avg("revenue_filled_avg_6_prior").over(w8))
)

matched_keys = (
    enriched_store_sales
    .select("id")
    .subtract(
        df_store_sales
        .filter(F.col("units").between(13, 38))
        .select("id")
    )
)

kept_rows = (
    enriched_store_sales
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .select(
        "id",
        "units",
        "revenue",
        "revenue_filled",
        "revenue_whole",
        "sqft",
        "revenue_filled_avg_6_prior",
        "last_known_revenue_whole",
        "running_units_2",
    )
)
