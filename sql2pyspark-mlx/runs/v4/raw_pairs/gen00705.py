from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc_nulls_first(), "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("last_restock").desc(), "id")

base = (
    df_inventory
    .filter(F.col("last_restock") <= "2023-10-01")
    .withColumn("quantity_scaled", F.round((F.col("quantity") * 1.07), 2))
)

with_dims = (
    base
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.category"), F.col("p.cost"))
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

joined = (
    latest
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .select(
        "a.*",
        F.col("d.category").alias("product_category"),
        F.col("d.product_name"),
    )
)

ranked = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    ranked
    .filter(F.col("last_restock") > F.lit("2024-04-26").cast("date"))
    .withColumn("dedup_rank_whole", F.ceil("dedup_rank"))
)

pivoted = (
    filtered
    .groupBy("product_id")
    .pivot("product_category", ["Home", "Sports"])
    .agg(F.max("dedup_rank_whole"))
    .select("product_id", F.col("Home").alias("home"), F.col("Sports").alias("sports"))
)

result = (
    pivoted
    .select("product_id", "home", "sports")
    .orderBy(F.col("product_id").desc())
    .limit(5)
)
