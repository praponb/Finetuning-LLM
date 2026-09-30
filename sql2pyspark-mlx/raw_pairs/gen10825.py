from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w_hist = Window.partitionBy(F.date_trunc("week", "signup_date"), "segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("country").orderBy(F.col("age").asc_nulls_first())

metrics = (
    df_customers
    .select(
        "*",
        F.corr("age", "credit_limit").over(w).alias("corr_age_credit_limit"),
        F.first("city", ignorenulls=True).over(w_hist).alias("first_known_city"),
        F.dense_rank().over(w3).alias("age_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "signup_date",
        "is_active",
        "age",
        "corr_age_credit_limit",
        F.when(F.col("country").rlike("^(JP|SG)$"), "PRIORITY").when(F.col("country").rlike("^(TH|US)$"), "CORE").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("id").asc_nulls_first(), F.col("age").desc())
    .limit(10)
)
