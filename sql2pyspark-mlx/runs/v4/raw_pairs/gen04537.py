from pyspark.sql import functions as F

result = (
    df_customers
    .select(
        "id",
        "country",
        "name",
        F.date_format("signup_date", "yyyy-MM-dd").alias("signup_date_label"),
    )
    .orderBy("country")
)
