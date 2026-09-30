from pyspark.sql import functions as F

valid = (
    df_warehouses
    .filter((F.col("city") == "Kuala Lumpur") & (F.length(F.trim(F.col("wh_code"))) >= 7))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
    )
)

without_matches = (
    valid.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.qty") < 275), "left_anti")
)

result = (
    without_matches
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        F.initcap(F.trim(F.col("wh_code"))).alias("wh_code_clean"),
    )
    .orderBy(F.col("wh_code_clean").desc(), F.col("capacity").desc())
)
