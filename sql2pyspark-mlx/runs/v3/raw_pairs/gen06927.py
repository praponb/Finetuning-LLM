from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "segment").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy(F.col("signup_date").desc())

result = (
    df_customers
    .filter(~F.col("segment").isin(["SMB", "Consumer", "Corporate"]))
    .select(
        "id",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "country",
        F.sum("credit_limit").over(w).alias("running_credit_limit"),
        F.rank().over(w2).alias("rnk"),
    )
    .orderBy(F.col("id").desc())
)
