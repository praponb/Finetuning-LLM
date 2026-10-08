"""Execution check for the v16 pairs: ../complex_v8/verify.py unchanged, with the synthetic catalog installed in the
main process and in every spawned worker.

  python verify_v16.py --src raw_v16 --workers 5   -> raw_v16/verified.jsonl, verify_report.json, verify_failures.jsonl
"""
import cx_patch

if __name__ == "__main__":
    cx_patch.install(None)
    import verify as cv      # complex_v8/verify.py
    cv._init_worker = cx_patch.init_verify_worker
    cv.main()
