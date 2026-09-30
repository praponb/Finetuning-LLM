from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").between(6900, 10500) | F.col("email").like("%@startup.io"))
    .select(
        "id",
        "city",
        "email",
        "country",
        F.concat("name", F.lit("/"), "country").alias("name_country"),
    )
    .orderBy("city", "id")
    .limit(50)
)
