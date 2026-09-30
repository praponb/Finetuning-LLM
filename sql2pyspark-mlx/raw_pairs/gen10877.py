from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("signup_date", "id").rowsBetween(-6, -1)
w2 = Window.orderBy(F.col("age").desc_nulls_first())

result = (
    df_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        F.avg("credit_limit").over(w).alias("credit_limit_avg_6_prior"),
        F.rank().over(w2).alias("age_rank"),
    )
)
