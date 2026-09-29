from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("credit_limit").desc(), "id")

result = (
    df_customers
    .select("id", "credit_limit", F.ntile(2).over(w).alias("credit_limit_ntile"))
    .orderBy("credit_limit_ntile")
)
