from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_rounded", "id")
w2 = Window.partitionBy("city", "product_id").orderBy("revenue_rounded_2", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy(F.year("sale_date"), "store_id")

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-03-03").cast("date"))
    .withColumn("revenue_rounded", F.round("revenue", 1))
    .withColumn("revenue_rounded_2", F.round("revenue", 1))
)

latest = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    latest
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.category"), F.col("p.product_name"), F.col("p.price"))
)

joined = (
    enriched
    .alias("src")
    .join(df_stores.alias("s"), F.col("src.store_id") == F.col("s.id"), "left")
    .filter(F.col("s.store_name").like("Si%"))
    .select("src.*", F.col("s.sqft"), F.col("s.store_name"), F.col("s.city"))
)

scored = (
    joined
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "revenue_rounded_2",
        "price",
        "store_name",
        "city",
        F.avg("sqft").over(w2).alias("sqft_avg_5_prior"),
        F.corr("revenue", "units").over(w3).alias("corr_revenue_units"),
    )
)

result = (
    scored
    .select("id", "sale_date", "revenue", "revenue_rounded_2", "price", "store_name")
)
