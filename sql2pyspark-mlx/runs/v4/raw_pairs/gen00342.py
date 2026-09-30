from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(F.year("signup_date") >= 2023)
    .select(
        "id",
        "country",
        "age",
        "city",
        "signup_date",
        "credit_limit",
        F.avg("age").over(w).alias("country_avg_age"),
        F.sum("credit_limit").over(w2).alias("running_credit_limit"),
    )
    .orderBy(F.col("country_avg_age").desc())
)
