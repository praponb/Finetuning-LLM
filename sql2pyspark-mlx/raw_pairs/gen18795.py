from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

result = (
    df_customers
    .select(
        "id",
        "segment",
        "signup_date",
        "credit_limit",
        "country",
        "email",
        (F.col("credit_limit") - F.lag("credit_limit", 1).over(w)).alias("credit_limit_change"),
        F.last("email").over(w2).alias("last_email"),
    )
)
