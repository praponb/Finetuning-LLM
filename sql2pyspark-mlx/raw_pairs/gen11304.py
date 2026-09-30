from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") != 4), "left_semi")
)

result = (
    not_flagged
    .select(
        "id",
        "product_name",
        "category",
        "launch_date",
        "tags",
        F.when(F.col("product_name").rlike("^(Desk Lamp|Backpack XL)$"), "PRIORITY").otherwise("EXTENDED").alias("product_name_zone"),
    )
)
