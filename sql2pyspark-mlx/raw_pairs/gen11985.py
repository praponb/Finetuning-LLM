from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country").orderBy(F.col("signup_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("country").orderBy(F.col("age").desc(), "id")

kept = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.order_date").between("2023-09-01", "2024-10-01")), "left_semi")
)

cleaned = (
    kept
    .filter((F.col("signup_date") > F.lit("2023-05-04").cast("date")) & F.col("segment").isNotNull())
    .withColumn("weeks_since_signup_date", (F.datediff(F.current_date(), "signup_date") / 7))
    .withColumn("city_group", F.when(F.col("city").isin(["Singapore", "London"]), "Core").otherwise("Other"))
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "name", "country", "is_active", "age", "weeks_since_signup_date")
)

deduped_customers = (
    deduped
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    deduped_customers
    .select("id", "name", "is_active", "weeks_since_signup_date")
    .orderBy("id")
)
