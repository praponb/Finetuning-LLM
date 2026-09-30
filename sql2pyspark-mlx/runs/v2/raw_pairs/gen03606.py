from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        F.percent_rank().over(w).alias("signup_date_pct_rank"),
    )
)
