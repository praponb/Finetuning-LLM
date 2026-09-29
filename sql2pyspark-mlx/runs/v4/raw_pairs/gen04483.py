from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.orderBy("age", "id")
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc(), "id")

scored = (
    df_customers
    .withColumn("age_bucket", F.ntile(10).over(w_recent))
    .withColumn("country_min_age", F.min("age").over(w2))
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

eligible = (
    latest
    .alias("s")
    .join(df_sessions.alias("x"), (F.col("x.customer_id") == F.col("s.id")) & (F.col("x.country") == "US"), "left_semi")
)

result = (
    eligible
    .select(
        "id",
        "email",
        "country",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        "age_bucket",
        "country_min_age",
        F.when(F.col("country").rlike("^(US|DE)$"), "OTHER_ZONE").when(F.col("country").rlike("^(TH|SG)$"), "CORE").otherwise("EXTENDED").alias("country_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first())
)
