from pyspark.sql import functions as F

valid_holders = df_policyholders.filter(F.col("holder_name").isNotNull())

holder_side = (
    valid_holders
    .select(
        F.lit("holder").alias("src"),
        F.col("id").alias("key_id"),
        F.col("region").alias("name"),
    )
)

holder_side_2 = (
    df_policyholders
    .filter((F.upper(F.trim(F.col("holder_name"))).like("A%")) & (F.col("segment") == "VIP"))
    .select(F.lit("holder_2").alias("src"), "id", "segment")
)

result = (
    holder_side
    .union(holder_side_2)
    .orderBy("src", "name")
)
