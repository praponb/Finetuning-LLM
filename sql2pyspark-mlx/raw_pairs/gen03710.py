from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.date_trunc("MONTH", "signup_date"))
w2 = Window.partitionBy(F.year("signup_date"), "segment").orderBy("credit_limit_rounded", "id")
w3 = Window.partitionBy(F.year("signup_date"), "city")
w4 = Window.orderBy("credit_limit_rounded", "id")
w5 = Window.orderBy(F.col("credit_limit_rounded").desc_nulls_last())

base = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-03-18").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.initcap(F.trim("name")).alias("name_title"),
        F.round("credit_limit", 1).alias("credit_limit_rounded"),
        F.coalesce("segment", F.lit("UNKNOWN")).alias("segment_clean"),
    )
)

result = (
    base
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("signup_date_year_max_credit_limit_rounded", F.max("credit_limit_rounded").over(w3))
    .withColumn("next_credit_limit", F.lead("credit_limit", 2).over(w4))
    .withColumn("credit_limit_rounded_rank", F.rank().over(w5))
)
