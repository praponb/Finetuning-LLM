from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.last_restock") < F.col("s.launch_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "tags",
        "supplier_id",
        F.when(F.col("product_name").rlike("^(Laptop Pro 14|Smart Watch)$"), "GROUP_A").when(F.col("product_name").rlike("^(wireless mouse|USB-C Hub)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("product_name_zone"),
    )
)
