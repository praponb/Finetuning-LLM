from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id")
w2 = Window.partitionBy("product_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

with_dims = (
    df_store_sales
    .alias("src")
    .join(df_stores.alias("d"), F.col("src.store_id") == F.col("d.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        F.col("d.sqft"),
        F.col("d.opened_date"),
    )
)

enriched = (
    with_dims
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.sale_date",
        "src.sqft",
        F.col("p.price"),
        F.col("p.cost"),
        F.col("p.product_name"),
    )
)

valid = (
    enriched
    .filter(F.col("sqft") >= 6286)
    .select(
        "id",
        "product_id",
        "price",
        "cost",
        "product_name",
        F.when(F.col("cost") >= 750, "High").when(F.col("cost") >= 360, "Medium").otherwise("Low").alias("cost_band"),
        F.greatest("cost", "price").alias("greatest_cost_price"),
    )
)

enriched_store_sales = (
    valid
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.category") == "Books")
    .select(
        "e.id",
        "e.product_id",
        "e.price",
        "e.cost",
        "e.product_name",
        "e.cost_band",
        F.col("d.product_name").alias("product_product_name"),
    )
)

with_windows = (
    enriched_store_sales
    .withColumn("product_id_min_price", F.min("price").over(w))
    .withColumn("first_known_price", F.first("price", ignorenulls=True).over(w2))
)

result = (
    with_windows
    .select(
        "id",
        "product_name",
        "cost_band",
        "product_id_min_price",
        "first_known_price",
        F.when(F.col("product_name").rlike("^(Running Shoes|USB-C Hub)$"), "CORE").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "PRIORITY").otherwise("GROUP_B").alias("product_name_zone"),
    )
)
