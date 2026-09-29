from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment", "city").orderBy(F.col("signup_date").desc(), "id")

deduped = (
    df_customers
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .select(
        "id",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "credit_limit",
    )
    .orderBy(F.col("signup_date").desc_nulls_first(), F.col("segment").desc())
)
