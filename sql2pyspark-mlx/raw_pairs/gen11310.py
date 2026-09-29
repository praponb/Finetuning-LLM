from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "New York") | (F.col("city") == "Berlin"))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.initcap(F.trim("country")).alias("country_title"),
    )
    .orderBy("country", F.col("rating").desc())
)
