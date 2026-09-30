from pyspark.sql import functions as F

cleaned_skus = (
    df_skus
    .filter((F.upper(F.trim(F.col("sku_code"))).like("E%")) | (F.col("is_active")))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
        F.when(F.col("weight_kg") >= 14.6, "large").when(F.col("weight_kg") >= 6.4, "medium").otherwise("small").alias("weight_kg_tier"),
    )
)

exploded_detail_moves = (
    cleaned_skus.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.sku_code",
        "s.category",
        "s.unit_cost",
        "s.weight_kg",
        "s.weight_kg_tier",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

tagged = exploded_detail_moves.withColumn("move_type_clean", F.lower(F.trim(F.col("move_type"))))

result = (
    tagged
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "weight_kg_tier",
        "move_id",
        "move_type",
        "move_moved_at",
        "move_operator",
        "move_unit_price",
        "move_type_clean",
        (F.col("weight_kg") + F.col("unit_cost")).alias("weight_kg_plus_unit_cost"),
    )
    .orderBy(
        F.col("move_operator").asc(),
        F.col("sku_code").desc(),
        "id",
        "move_id",
    )
    .limit(10)
)
