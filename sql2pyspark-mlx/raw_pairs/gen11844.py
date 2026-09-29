from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipping_fee").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("order_status", "segment").orderBy(F.col("credit_limit").desc_nulls_last(), "id")
w3 = Window.partitionBy("city").orderBy(F.col("days_since_signup_date").desc_nulls_last(), "id")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())

with_latest = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status"))
)

ranked = (
    with_latest
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

valid = (
    ranked
    .filter((F.col("signup_date") > F.lit("2023-07-15").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        "order_status",
        F.datediff(F.current_date(), "signup_date").alias("days_since_signup_date"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_latest_customers = (
    deduped
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.status").alias("order_status_2"))
)

result = (
    with_latest_customers
    .select(
        "id",
        "name",
        "email",
        "country",
        "segment",
        "is_active",
        "age",
        F.when(F.col("country").rlike("^(JP|UK)$"), "GROUP_A").otherwise("OTHER_ZONE").alias("country_zone"),
    )
    .orderBy(F.col("age").asc_nulls_last())
)
