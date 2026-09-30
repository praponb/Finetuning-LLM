from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") < 12)
    .select(
        "id",
        "city",
        F.initcap(F.trim("supplier_name")).alias("supplier_name_title"),
        F.ceil("rating").alias("rating_whole"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
