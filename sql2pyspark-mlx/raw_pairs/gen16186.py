from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy("credit_limit", "id")
w2 = Window.partitionBy("country")

metrics = (
    df_customers
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.lead("age", 2).over(w).alias("next_age"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share"),
    )
)

result = (
    metrics
    .select("id", "signup_date", "is_active", "age")
    .orderBy("id", "age")
    .limit(5)
)
