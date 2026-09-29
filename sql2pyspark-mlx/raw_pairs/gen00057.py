from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("product_id").isin([5, 9]))
    .select("id", "quantity")
    .unionByName(
        df_inventory
        .filter(F.col("product_id").isin([3, 4]))
        .select("id", "quantity")
    )
)
