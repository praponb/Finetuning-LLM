from pyspark.sql import functions as F

enriched = (
    df_order_items
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.product_name"), F.col("d.launch_date"))
)

result = (
    enriched
    .select(
        "id",
        "product_id",
        "unit_price",
        "product_name",
        "launch_date",
        F.when(F.col("product_name").rlike("^(USB-C Hub|Backpack XL)$"), "OTHER_ZONE").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "CORE").otherwise("EXTENDED").alias("product_name_zone"),
    )
    .orderBy(F.col("id").desc())
)
