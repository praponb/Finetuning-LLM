from pyspark.sql import functions as F

result = (
    df_inventory
    .filter((F.col("last_restock") > "2023-10-12") & F.col("product_id").isin([1, 3, 5, 7]) & F.col("product_id").isin([2, 3, 7]))
    .select(
        "id",
        "quantity",
        F.date_trunc("WEEK", "last_restock").alias("last_restock_period"),
    )
    .orderBy("quantity")
)
