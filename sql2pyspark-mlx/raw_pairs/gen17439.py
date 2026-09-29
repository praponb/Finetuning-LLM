from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select("id", "sale_date", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([3, 4, 9]))
        .select("id", "sale_date", "store_id")
    )
    .distinct()
)
