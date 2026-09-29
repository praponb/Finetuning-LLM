from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "TH") & (~F.col("city").isin(["London", "New York"]) | (F.length("supplier_name") < 5)))
    .select(
        "id",
        "rating",
        "country",
        "supplier_name",
        F.concat_ws("/", "country", "city").alias("country_city_key"),
        F.floor("rating").alias("rating_whole"),
    )
)
