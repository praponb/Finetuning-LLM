from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")

result = (
    df_customers
    .select(
        "id",
        "segment",
        "is_active",
        "age",
        F.corr("credit_limit", "age").over(w).alias("corr_credit_limit_age"),
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
    )
)
