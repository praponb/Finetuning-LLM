from pyspark.sql import functions as F

base = (
    df_warehouses
    .filter(F.col("capacity") < 11506)
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.when(F.col("city").isin("Singapore", "Bangkok"), "group_a").when(F.col("city") == "Kuala Lumpur", "group_b").otherwise("other").alias("city_group"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
    )
)

exploded_detail_moves = (
    base.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.capacity",
        "s.opened_on",
        "s.city_group",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    exploded_detail_moves
    .groupingSets(
        [["move_operator", "move_id", "city_group"], ["move_operator"], ["move_id"], []],
        "move_operator", "move_id", "city_group",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .select(
        F.coalesce(F.col("move_operator"), F.lit("ALL")).alias("move_operator"),
        "move_id",
        F.coalesce(F.col("city_group"), F.lit("ALL")).alias("city_group"),
        "gid",
        "distinct_ids",
    )
    .orderBy(
        F.col("move_operator").asc_nulls_last(),
        F.col("move_id").asc_nulls_last(),
        F.col("city_group").asc_nulls_last(),
    )
)
