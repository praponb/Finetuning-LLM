from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("price").desc(), "id")

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-03-28").cast("date")) & (F.col("cost") <= 420))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "launch_date",
        "supplier_id",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
        F.trim("brand").alias("clean_brand"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

ranked = (
    base
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

remaining_keys = (
    ranked
    .select("category", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.length("product_name") > 11)
        .select("category", "supplier_id")
    )
)

kept_rows = (
    ranked
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

filtered = (
    kept_rows
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-09-05"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "supplier_id",
        "brand_filled",
        F.lower("category").alias("category_lower"),
    )
)

matched_keys = (
    filtered
    .select("brand", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("price").isNull())
        .select("brand", "supplier_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("brand")
    .pivot("category", ["Grocery", "Home", "Books"])
    .agg(F.max("price").alias("amount"))
)
