from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("age", "id")
w2 = Window.partitionBy("city", "country").orderBy("credit_limit", "id")
w3 = Window.orderBy(F.col("age").desc(), "id")

enriched = (
    df_customers
    .withColumn("next_credit_limit", F.lead("credit_limit", 1).over(w))
    .withColumn("next_age", F.lead("age", 1).over(w2))
    .withColumn("age_bucket", F.ntile(10).over(w3))
)

result = (
    enriched
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        "next_credit_limit",
    )
    .orderBy(F.col("id").desc())
    .limit(5)
)
