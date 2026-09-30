from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .filter(F.col("credit_limit") < 5200)
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.lead("credit_limit", 1).over(w).alias("next_credit_limit"),
        F.last("signup_date").over(w2).alias("last_signup_date"),
    )
    .orderBy(F.col("next_credit_limit").desc())
)
