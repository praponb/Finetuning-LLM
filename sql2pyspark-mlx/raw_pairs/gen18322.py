from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date")

result = (
    df_customers
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        F.rank().over(w).alias("signup_date_rank"),
    )
    .orderBy("signup_date", "signup_date_rank")
)
