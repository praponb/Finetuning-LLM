from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.lit("2023-08-27").cast("date")) & F.month("last_restock").isin([1, 4, 10]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_format("last_restock", "yyyy-MM").alias("last_restock_label"),
    )
)

enriched = (
    base
    .withColumn("last3_count_quantity", F.count("quantity").over(w))
    .withColumn("last3_max_quantity", F.max("quantity").over(w))
    .withColumn("last3_sum_quantity", F.sum("quantity").over(w))
)

enriched_inventory = (
    enriched
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "inner")
    .select(
        "src.id",
        "src.product_id",
        "src.last_restock",
        "src.last_restock_label",
        "src.last3_max_quantity",
        "src.last3_sum_quantity",
        F.col("p.category"),
        F.col("p.product_name"),
    )
)

joined = (
    enriched_inventory
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.price") <= 370)
    .select(
        "a.id",
        "a.product_id",
        "a.last_restock",
        "a.last_restock_label",
        "a.last3_max_quantity",
        "a.category",
        "a.product_name",
        F.col("d.stock_qty"),
    )
)

joined_inventory = (
    joined
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.last_restock",
        "src.last_restock_label",
        "src.last3_max_quantity",
        "src.category",
        "src.stock_qty",
        F.col("p.launch_date"),
        F.col("p.brand"),
        F.col("p.price"),
    )
)

result = (
    joined_inventory
    .select(
        "id",
        "last_restock_label",
        "last3_max_quantity",
        "category",
        "launch_date",
        "brand",
        F.when(F.col("category").rlike("^(Electronics|Fashion)$"), "EXTENDED").when(F.col("category").rlike("^(Home|Sports)$"), "PRIORITY").otherwise("GROUP_A").alias("category_zone"),
    )
    .orderBy(F.col("brand").desc_nulls_first())
)
