from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([3, 26, 28]))
    .select("id", "launch_date")
    .unionByName(
        df_products
        .filter(F.col("price").between(410, 860))
        .select("id", "launch_date")
    )
    .distinct()
)
