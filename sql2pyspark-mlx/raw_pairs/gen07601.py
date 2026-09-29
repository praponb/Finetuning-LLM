from pyspark.sql import functions as F

not_flagged = (
    df_stores
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") < F.col("s.opened_date")), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.when(F.col("region").rlike("^(East|Central)$"), "GROUP_A").otherwise("EXTENDED").alias("region_zone"),
    )
    .orderBy(F.col("region").desc())
)
