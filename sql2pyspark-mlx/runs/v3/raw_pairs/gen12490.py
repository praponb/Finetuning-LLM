from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.length("supplier_name") >= 11)
    .select(
        "id",
        "rating",
        "country",
        "city",
        F.initcap(F.trim("city")).alias("city_title"),
    )
    .orderBy("city", F.col("rating").desc())
)
