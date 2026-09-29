from pyspark.sql import functions as F

scoped = (
    df_policies
    .filter(F.col("premium").between(1541.8, 4417.0))
    .select(
        "id",
        "holder_id",
        "product",
        "end_date",
        "status",
    )
)

with_details = (
    scoped.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.end_date",
        "s.status",
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_parent = (
    with_details.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select("s.*", F.col("p.region").alias("holder_region"))
)

result = (
    with_parent
    .groupingSets(
        [["holder_id", "holder_region", "product"], ["holder_id"], ["holder_region"], []],
        "holder_id", "holder_region", "product",
    )
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.min("end_date").alias("first_end_date"),
        F.count(F.when(F.col("status").isin("EXPIRED", "CANCELLED"), 1)).alias("n_status_in_expired_cancelled"),
    )
    .orderBy(
        F.col("holder_id").asc_nulls_last(),
        F.col("holder_region").asc_nulls_last(),
        F.col("product").asc_nulls_last(),
    )
)
