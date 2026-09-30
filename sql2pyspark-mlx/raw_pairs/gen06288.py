from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("segment")
w4 = Window.partitionBy("city").orderBy(F.col("credit_limit").desc_nulls_first())

result = (
    df_customers
    .filter(F.col("email").isNull())
    .withColumn("corr_credit_limit_age", F.corr("credit_limit", "age").over(w))
    .withColumn("first_known_segment", F.first("segment", ignorenulls=True).over(w2))
    .withColumn("corr_credit_limit_age_2", F.corr("credit_limit", "age").over(w3))
    .withColumn("credit_limit_cume_dist", F.cume_dist().over(w4))
)
