from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3800)
    .select("id", "product_id", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.month("sale_date").isin([4, 6, 8]))
        .select("id", "product_id", "store_id")
    )
)
