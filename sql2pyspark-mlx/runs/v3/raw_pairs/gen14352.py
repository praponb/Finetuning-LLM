from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("signup_date", "id")

result = (
    df_customers
    .filter(~F.col("email").like("%@startup.io"))
    .select("id", "city", "signup_date", F.row_number().over(w).alias("pos"))
    .filter(F.col("pos") == 1)
    .select("id", "city", "signup_date")
)
