from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")
w2 = Window.partitionBy("segment").orderBy("signup_date")

result = (
    df_customers
    .select(
        "id",
        "city",
        "signup_date",
        "segment",
        F.row_number().over(w).alias("row_num"),
        F.percent_rank().over(w2).alias("signup_date_pct_rank"),
    )
)
