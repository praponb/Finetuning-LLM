from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

ranked = (
    df_inventory
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

filtered = (
    ranked
    .filter((F.col("last_restock") >= F.lit("2023-09-20").cast("date")) & F.col("product_id").isin([3, 4, 5, 6]))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_inventory
        .filter(F.col("product_id").isin([7, 9, 10]))
        .select("id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    verified
    .filter(F.col("last_restock") >= F.lit("2023-09-11").cast("date"))
    .withColumn("quantity_whole_whole", F.floor("quantity_whole"))
    .withColumn("quantity_whole_plus_quantity", (F.col("quantity_whole") + F.col("quantity")))
)

enriched = (
    base
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.cost"), F.col("p.stock_qty"), F.col("p.category"))
)

result = (
    enriched
    .groupBy(
        "category",
        F.when(F.col("quantity_whole_plus_quantity") >= 85, "PREMIUM").otherwise("STANDARD").alias("quantity_whole_plus_quantity_tier"),
    )
    .agg(
        F.round(F.sum("quantity_whole_whole") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("quantity_whole_whole_per_home"),
        F.count("*").alias("num_rows"),
        F.count(F.when(F.col("category") == "Fashion", 1)).alias("fashion_rows"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .filter(F.col("num_rows") >= 1)
)
