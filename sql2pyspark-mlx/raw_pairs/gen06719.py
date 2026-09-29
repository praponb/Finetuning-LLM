from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city", "country").orderBy("age", "id")
w_recent = Window.partitionBy("country", "city")
w3 = Window.partitionBy("country")

result = (
    df_customers
    .filter(~F.col("is_active"))
    .select(
        "id",
        "name",
        "email",
        "signup_date",
        "is_active",
        "credit_limit",
        "age",
        F.ntile(5).over(w_grp).alias("age_bucket"),
        F.corr("credit_limit", "age").over(w_recent).alias("corr_credit_limit_age"),
        F.avg("age").over(w3).alias("country_avg_age"),
    )
)
