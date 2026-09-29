from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "signup_date",
        "country",
        "name",
        F.regexp_replace("name", " ", "_").alias("name_slug"),
    )
    .orderBy("name_slug", "id")
    .limit(50)
)
