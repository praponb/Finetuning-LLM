from pyspark.sql import functions as F

result = (
    df_accounts.alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .filter(F.col("c.name").like("%Cha%"))
    .groupBy(
        F.when(F.col("c.city").isin(["Bangkok", "Tokyo"]), "group_a").otherwise("other").alias("city_group"),
        F.col("c.country"),
    )
    .agg(
        (F.max(F.col("a.balance")) - F.min(F.col("a.balance"))).alias("balance_range"),
        F.max(F.col("a.opened_date")).alias("last_opened_date"),
    )
    .orderBy(F.col("city_group").desc(), "country")
    .limit(20)
)
