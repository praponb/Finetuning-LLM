from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") >= 10)
    .select(
        "id",
        "city",
        "name",
        "is_active",
        F.upper("country").alias("country_upper"),
        F.concat("name", F.lit(" "), "city").alias("name_city"),
    )
)
