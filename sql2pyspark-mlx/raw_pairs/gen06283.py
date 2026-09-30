from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date_month_end", "id")
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("balance").asc_nulls_first(), "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-09-20").cast("date")) & F.col("customer_id").isin([3, 11, 18]))
    .withColumn("opened_date_month_end", F.last_day("opened_date"))
)

scored = (
    cleaned
    .withColumn("next_balance", F.lead("balance").over(w))
    .withColumn("prev_balance", F.lag("balance").over(w))
    .withColumn("seq_no", F.row_number().over(w))
)

joined = (
    scored
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.id",
        "e.customer_id",
        "e.balance",
        "e.status",
        "e.opened_date_month_end",
        "e.next_balance",
        "e.seq_no",
        F.col("c.country"),
        F.col("c.age"),
    )
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

wide = (
    ranked
    .groupBy("status")
    .pivot("country", ["DE", "UK"])
    .agg(F.max("seq_no").alias("total"))
)

result = (
    wide
    .select(
        "status",
        "DE",
        F.when(F.col("status").rlike("^(CLOSED|FROZEN)$"), "EXTENDED").when(F.col("status").rlike("^(OPEN)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("status").asc_nulls_first(), F.col("DE").desc())
)
