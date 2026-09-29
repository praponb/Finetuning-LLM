from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.month("signup_date").isin([2, 3, 5]) & (~F.col("email").like("%@mail.co.th") | F.col("age").isNotNull()))
    .select(
        "id",
        "segment",
        "email",
        F.when(F.col("country") == "UK", 1).otherwise(0).alias("is_uk"),
        F.regexp_replace("name", " ", "_").alias("name_slug"),
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
