from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("credit_limit").desc())

result = (
    df_customers
    .filter(F.col("email").like("%@company.co"))
    .select("id", "city", "credit_limit", "segment", F.rank().over(w).alias("rnk"))
    .filter(F.col("rnk") == 1)
    .select("id", "city", "credit_limit", "segment")
)
