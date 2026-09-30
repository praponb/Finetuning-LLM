from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("segment").orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country")

result = (
    df_customers
    .select(
        "id",
        "name",
        "country",
        "segment",
        "signup_date",
        "age",
        F.sum("age").over(w).alias("running_age"),
        F.round((F.col("credit_limit") / F.sum("credit_limit").over(w2)), 4).alias("credit_limit_share"),
    )
)
