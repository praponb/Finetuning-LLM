from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") > 690)
    .select("product_name")
    .unionByName(
        df_products
        .filter(F.col("brand").isin(["Globex", "Initech", "Stark"]))
        .select("product_name")
    )
    .distinct()
)
