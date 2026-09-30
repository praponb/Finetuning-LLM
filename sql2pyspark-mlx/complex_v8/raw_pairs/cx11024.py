from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter(F.col("city") != "Bangkok")
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.date_format(F.col("opened_on"), "yyyy-MM-dd").alias("opened_on_label"),
        F.date_format(F.col("opened_on"), "yyyy-MM-dd").alias("opened_on_label_2"),
    )
)

without_matches = (
    cleaned_warehouses.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.month(F.col("st.moved_at")).isin(1, 2)) & (F.col("st.move_type").isin("TRANSFER", "INBOUND", "OUTBOUND")), "left_anti")
)

filtered = (
    without_matches.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.sku_id").isNotNull()), "left_anti")
)

result = (
    filtered
    .select(
        "id",
        "wh_code",
        "opened_on_label",
        F.length(F.trim(F.col("opened_on_label_2"))).alias("opened_on_label_2_len"),
    )
    .orderBy(
        F.col("wh_code").desc(),
        F.col("opened_on_label_2_len").asc_nulls_last(),
        "id",
    )
    .limit(10)
)
