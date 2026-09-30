from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Sia%") & ~F.col("supplier_name").like("%Glob%"))
    .select(
        "id",
        "rating",
        "city",
        "country",
        F.initcap(F.trim("city")).alias("city_title"),
    )
    .orderBy("city", F.col("rating").desc())
)
