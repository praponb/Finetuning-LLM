from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.orderBy("credit_limit", "id")
w2 = Window.partitionBy("segment", "city")
w3 = Window.partitionBy("country", "segment").orderBy(F.col("signup_date").desc(), "id")

result = (
    df_customers
    .select(
        "*",
        F.ntile(4).over(w_part).alias("credit_limit_bucket"),
        F.corr("age", "credit_limit").over(w2).alias("corr_age_credit_limit"),
        F.row_number().over(w3).alias("seq_no"),
    )
)
