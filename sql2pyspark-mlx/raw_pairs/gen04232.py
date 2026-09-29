from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy(F.year("signup_date"), "country").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.col("is_active"))
    .select(
        "id",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.min("credit_limit").over(w).alias("last4_min_credit_limit"),
        F.sum("credit_limit").over(w).alias("last4_sum_credit_limit"),
        F.max("credit_limit").over(w).alias("last4_max_credit_limit"),
        F.lag("age", 2).over(w2).alias("prev_age"),
    )
)
