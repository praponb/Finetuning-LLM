from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") < 14300) & (F.col("city") == "Berlin"))
    .select(
        "id",
        "name",
        "city",
        "signup_date",
        F.substring("country", 1, 2).alias("country_prefix"),
        F.substring("country", 1, 3).alias("country_prefix_2"),
    )
    .orderBy("country_prefix")
)
