from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select("id", "launch_date", "brand")
    .unionByName(
        df_products
        .filter(F.col("cost") > 530)
        .select("id", "launch_date", "brand")
    )
    .orderBy(F.col("brand").desc())
)
