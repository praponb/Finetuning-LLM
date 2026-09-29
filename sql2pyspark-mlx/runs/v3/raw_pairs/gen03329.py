from pyspark.sql import functions as F

result = (
    df_products
    .filter(~F.col("category").isin(["Sports", "Grocery", "Books"]) & (F.col("cost") < 420))
    .select(
        "id",
        "category",
        "cost",
        "supplier_id",
        F.round((F.col("cost") * 100), 2).alias("cost_scaled"),
    )
)
